use super::{request_builder::RequestBuilder, HANDLER_TYPE};
use chrono::DateTime;
use minidom::Element;
use rslex_core::{
    file_io::{StreamError, StreamResult},
    MapErrToUnknown, StreamInfo, SyncRecord,
};
use rslex_http_stream::SessionPropertiesExt;
use std::{collections::HashMap, str::FromStr};

const HDI_ISFOLDER_METADATA_NAME: &'static str = "hdi_isfolder";

#[derive(Debug, Clone)]
/// Blob metadata as retrieved from the blob service
pub(super) struct Blob {
    pub name: String,

    pub properties: HashMap<String, String>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum BlobEntry {
    Blob(Blob),
    BlobPrefix(String),
}

impl PartialEq<Blob> for Blob {
    /// Only used for test. skip properties comparing
    fn eq(&self, rhs: &Blob) -> bool {
        self.name == rhs.name
    }
}

/// BlobList is the result list by calling "List Blob" from blob service
/// It's simply a collection of Blob
#[derive(Debug, PartialEq)]
pub(super) struct BlobList {
    pub blobs: Vec<BlobEntry>,

    pub next_marker: Option<String>,
}

/// the Element.get_child forces a namespace
/// but there could be elements without namespace
/// put a convenient function here
trait ElementWithoutNameSpace {
    fn get_child_without_ns<S: AsRef<str>>(&self, name: S) -> Option<&Element>;
}

impl ElementWithoutNameSpace for Element {
    fn get_child_without_ns<S: AsRef<str>>(&self, name: S) -> Option<&Element> {
        self.children().find(|c| c.name() == name.as_ref())
    }
}

impl FromStr for BlobList {
    type Err = StreamError;

    fn from_str(s: &str) -> StreamResult<Self> {
        let root: Element = s.parse::<Element>().map_err_to_unknown()?;
        let mut blobs = vec![];

        for b in root
            .get_child_without_ns("Blobs")
            .ok_or(StreamError::Unknown("BlobList XML missing <Blobs> node".to_owned(), None))?
            .children()
        {
            blobs.push(if b.name() == "Blob" {
                BlobEntry::Blob(Blob {
                    name: b
                        .get_child_without_ns("Name")
                        .ok_or(StreamError::Unknown("BlobList XML <Blob> missing <Name> node".to_owned(), None))?
                        .text(),
                    properties: match b.get_child_without_ns("Properties") {
                        Some(e) => e.children().map(|c| (c.name().to_owned(), c.text())).collect(),
                        None => HashMap::default(),
                    },
                    metadata: match b.get_child_without_ns("Metadata") {
                        Some(e) => e.children().map(|c| (c.name().to_owned(), c.text())).collect(),
                        None => HashMap::default(),
                    },
                })
            } else {
                BlobEntry::BlobPrefix(
                    b.get_child_without_ns("Name")
                        .ok_or(StreamError::Unknown(
                            "BlobList XML <BlobPrefix> missing <Name> node".to_owned(),
                            None,
                        ))?
                        .text(),
                )
            });
        }

        let next_marker = match root.get_child_without_ns("NextMarker") {
            Some(e) if e.text() == "" => None,
            Some(e) => Some(e.text()),
            None => None,
        };

        tracing::debug!("[BlobList::from_str()] BlobList deserialized, with {} blobs", blobs.len());

        Ok(BlobList { blobs, next_marker })
    }
}

#[derive(Debug, PartialEq)]
pub(super) enum BlockId {
    Committed(String),
    Uncommitted(String),
    Latest(String),
}

#[derive(Debug, PartialEq)]
pub(super) struct BlockList {
    pub(crate) blocks: Vec<BlockId>,
}

impl FromStr for BlockList {
    type Err = StreamError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let root: Element = s.parse::<Element>().expect("valid XML for BlockList");
        let mut blocks = vec![];

        for b in root.children() {
            let s = String::from_utf8(base64::decode(b.text()).unwrap()).unwrap();
            match b.name() {
                "Uncommitted" => {
                    blocks.push(BlockId::Uncommitted(s));
                },
                "Committed" => {
                    blocks.push(BlockId::Committed(s));
                },
                "Latest" => {
                    blocks.push(BlockId::Latest(s));
                },
                _ => panic!(),
            }
        }

        Ok(BlockList { blocks })
    }
}

impl ToString for BlockList {
    fn to_string(&self) -> String {
        let root = Element::builder("BlockList")
            .append_all(self.blocks.iter().map(|block| match block {
                BlockId::Committed(s) => Element::builder("Committed").append(base64::encode(s)),
                BlockId::Uncommitted(s) => Element::builder("Uncommitted").append(base64::encode(s)),
                BlockId::Latest(s) => Element::builder("Latest").append(base64::encode(s)),
            }))
            .build();

        let mut output = vec![];
        root.write_to(&mut output).unwrap();
        String::from_utf8(output).unwrap()
    }
}

impl Blob {
    pub(crate) fn to_stream_info(&self, request_builder: &RequestBuilder, arguments: SyncRecord) -> StreamInfo {
        let mut session_properties = HashMap::default();
        if let Some(length) = self.properties.get("Content-Length") {
            session_properties.set_size(length.parse::<u64>().unwrap());
        }
        if let Some(created_time) = self.properties.get("Creation-Time") {
            session_properties.set_created_time(DateTime::parse_from_rfc2822(created_time.as_str()).unwrap().into());
        }
        if let Some(modified_time) = self.properties.get("Last-Modified") {
            session_properties.set_modified_time(DateTime::parse_from_rfc2822(modified_time.as_str()).unwrap().into());
        }
        StreamInfo::new(HANDLER_TYPE, request_builder.path_to_uri(&self.name), arguments).with_session_properties(session_properties)
    }

    /// When blobfuse creates new directory it actually creates empty blob
    /// and sets hdi_isfolder = true to emulate folders when azure blob actually has no folders concepts.
    /// We skip this blobs in our listing.
    pub(crate) fn is_hdi_folder(&self) -> bool {
        self.metadata.get(HDI_ISFOLDER_METADATA_NAME).map_or(false, |m| m == "true")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use fluent_assertions::*;
    use maplit::hashmap;
    use std::sync::Arc;

    impl ToString for BlobList {
        fn to_string(&self) -> String {
            let root = Element::builder("EnumerationResults")
                .append(Element::builder("Blobs").append_all(self.blobs.iter().map(|blob_entry| {
                    match blob_entry {
                        BlobEntry::Blob(blob) => Element::builder("Blob")
                            .append(Element::builder("Name").append(blob.name.as_str()))
                            .append(
                                Element::builder("Properties")
                                    .append_all(blob.properties.iter().map(|kv| Element::builder(kv.0).append(kv.1.as_str()))),
                            )
                            .append(
                                Element::builder("Metadata")
                                    .append_all(blob.metadata.iter().map(|kv| Element::builder(kv.0).append(kv.1.as_str()))),
                            ),
                        BlobEntry::BlobPrefix(p) => Element::builder("BlobPrefix").append(Element::builder("Name").append(p.as_str())),
                    }
                })))
                .append(Element::builder("NextMarker").append(self.next_marker.as_ref().map(|s| s.as_str()).unwrap_or("")))
                .build();

            let mut output = vec![];
            root.write_to(&mut output).unwrap();
            String::from_utf8(output).unwrap()
        }
    }

    #[test]
    fn deserialize_bloblist_with_properties() {
        let expected = BlobList {
            blobs: vec![BlobEntry::Blob(Blob {
                name: "housing.xlsx".to_string(),
                properties: hashmap! {
                    "Creation-Time".to_owned() => "Sat, 13 Oct 2018 00:18:24 GMT".to_owned(),
                    "Last-Modified".to_owned() => "Sat, 13 Oct 2018 00:18:24 GMT".to_owned(),
                    "Etag".to_owned() => "0x8D630A163E98826".to_owned(),
                    "Content-Length".to_owned() => "12213".to_owned(),
                    "Content-Type".to_owned() => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet".to_owned(),
                    "Content-Encoding".to_owned() => "".to_owned(),
                    "Content-Language".to_owned() => "".to_owned(),
                    "Content-MD5".to_owned() => "oLxUKdd/MdPakYondJmsZQ==".to_owned(),
                    "Cache-Control".to_owned() => "".to_owned(),
                    "Content-Disposition".to_owned() => "".to_owned(),
                    "BlobType".to_owned() => "BlockBlob".to_owned(),
                    "AccessTier".to_owned() => "Cool".to_owned(),
                    "AccessTierInferred".to_owned() => "true".to_owned(),
                    "LeaseStatus".to_owned() => "unlocked".to_owned(),
                    "LeaseState".to_owned() => "available".to_owned(),
                    "ServerEncrypted".to_owned() => "true".to_owned(),
                },
                metadata: hashmap! {},
            })],
            next_marker: None,
        };

        r##"
        <?xml version="1.0" encoding="utf-8"?>
        <EnumerationResults ServiceEndpoint="https://dataprepe2etest.blob.core.windows.net/" ContainerName="test">
            <Blobs>
                <Blob>
                    <Name>housing.xlsx</Name>
                    <Properties>
                        <Creation-Time>Sat, 13 Oct 2018 00:18:24 GMT</Creation-Time>
                        <Last-Modified>Sat, 13 Oct 2018 00:18:24 GMT</Last-Modified>
                        <Etag>0x8D630A163E98826</Etag>
                        <Content-Length>12213</Content-Length>
                        <Content-Type>application/vnd.openxmlformats-officedocument.spreadsheetml.sheet</Content-Type>
                        <Content-Encoding />
                        <Content-Language />
                        <Content-MD5>oLxUKdd/MdPakYondJmsZQ==</Content-MD5>
                        <Cache-Control />
                        <Content-Disposition />
                        <BlobType>BlockBlob</BlobType>
                        <AccessTier>Cool</AccessTier>
                        <AccessTierInferred>true</AccessTierInferred>
                        <LeaseStatus>unlocked</LeaseStatus>
                        <LeaseState>available</LeaseState>
                        <ServerEncrypted>true</ServerEncrypted>
                    </Properties>
                </Blob>
            </Blobs>
        </EnumerationResults>
                    "##
        .parse::<BlobList>()
        .should()
        .be_ok()
        .with_value(expected);
    }

    #[test]
    fn deserialize_bloblist_with_metadata() {
        let expected = BlobList {
            blobs: vec![BlobEntry::Blob(Blob {
                name: "housing.xlsx".to_string(),
                properties: hashmap! {},
                metadata: hashmap! {
                    "metadata_property".to_owned() => "metadata_value".to_owned()
                },
            })],
            next_marker: None,
        };

        r##"
        <?xml version="1.0" encoding="utf-8"?>
        <EnumerationResults ServiceEndpoint="https://dataprepe2etest.blob.core.windows.net/" ContainerName="test">
            <Blobs>
                <Blob>
                    <Name>housing.xlsx</Name>
                    <Metadata>
                        <metadata_property>metadata_value</metadata_property>
                    </Metadata>
                </Blob>
            </Blobs>
        </EnumerationResults>
                    "##
        .parse::<BlobList>()
        .should()
        .be_ok()
        .with_value(expected);
    }

    #[test]
    fn deserialize_bloblist_without_properties() {
        let expected = BlobList {
            blobs: vec![
                BlobEntry::Blob(Blob {
                    name: "!_do_not_touch.csv".to_string(),
                    properties: HashMap::default(),
                    metadata: hashmap! {},
                }),
                BlobEntry::BlobPrefix("input/".to_string()),
            ],
            next_marker: Some("2!76!MDAwMDEyIWhvdXNpbmcueGxzeCEwMDAwMjghOTk5OS0xMi0zMVQyMzo1OTo1OS45OTk5OTk5WiE-".to_owned()),
        };

        r##"
        <?xml version="1.0" encoding="utf-8"?>
        <EnumerationResults ServiceEndpoint="https://dataprepe2etest.blob.core.windows.net/" ContainerName="test">
            <Blobs>
                <Blob>
                    <Name>!_do_not_touch.csv</Name>
                </Blob>
                <BlobPrefix>
                    <Name>input/</Name>
                </BlobPrefix>
            </Blobs>
            <NextMarker>2!76!MDAwMDEyIWhvdXNpbmcueGxzeCEwMDAwMjghOTk5OS0xMi0zMVQyMzo1OTo1OS45OTk5OTk5WiE-</NextMarker>
        </EnumerationResults>
                    "##
        .parse::<BlobList>()
        .should()
        .be_ok()
        .with_value(expected);
    }

    #[test]
    fn deserialize_bloblist_with_next_marker() {
        let expected = BlobList {
            blobs: vec![BlobEntry::Blob(Blob {
                name: "!_do_not_touch.csv".to_string(),
                properties: HashMap::default(),
                metadata: hashmap! {},
            })],
            next_marker: Some("2!76!MDAwMDEyIWhvdXNpbmcueGxzeCEwMDAwMjghOTk5OS0xMi0zMVQyMzo1OTo1OS45OTk5OTk5WiE-".to_owned()),
        };

        r##"
        <?xml version="1.0" encoding="utf-8"?>
        <EnumerationResults ServiceEndpoint="https://dataprepe2etest.blob.core.windows.net/" ContainerName="test">
            <Blobs>
                <Blob>
                    <Name>!_do_not_touch.csv</Name>
                </Blob>
            </Blobs>
            <NextMarker>2!76!MDAwMDEyIWhvdXNpbmcueGxzeCEwMDAwMjghOTk5OS0xMi0zMVQyMzo1OTo1OS45OTk5OTk5WiE-</NextMarker>
        </EnumerationResults>
        "##
        .parse::<BlobList>()
        .should()
        .be_ok()
        .with_value(expected);
    }

    #[test]
    fn deserialize_bloblist_without_next_marker() {
        let expected = BlobList {
            blobs: vec![BlobEntry::Blob(Blob {
                name: "!_do_not_touch.csv".to_string(),
                properties: HashMap::default(),
                metadata: hashmap! {},
            })],
            next_marker: None,
        };

        r##"
        <EnumerationResults>
            <Blobs>
                <Blob>
                    <Name>!_do_not_touch.csv</Name>
                </Blob>
            </Blobs>
            <NextMarker/>
        </EnumerationResults>"##
            .parse::<BlobList>()
            .should()
            .be_ok()
            .with_value(expected);
    }

    #[test]
    fn deserialize_empty_from_xml() {
        let expected = BlobList {
            blobs: vec![],
            next_marker: None,
        };

        r##"
        <?xml version="1.0" encoding="utf-8"?>
        <EnumerationResults ServiceEndpoint="https://dataprepe2etest.blob.core.windows.net/" ContainerName="test">
            <Blobs>
            </Blobs>
        </EnumerationResults>
                    "##
        .parse::<BlobList>()
        .should()
        .be_ok()
        .with_value(expected);
    }

    #[test]
    fn serialize_bloblist_to_xml() {
        let expected = BlobList {
            blobs: vec![
                BlobEntry::Blob(Blob {
                    name: "!_do_not_touch.csv".to_owned(),
                    properties: HashMap::default(),
                    metadata: hashmap! {},
                }),
                BlobEntry::BlobPrefix("input/".to_owned()),
            ],
            next_marker: Some("marker".to_owned()),
        };

        expected.to_string().parse::<BlobList>().should().be_ok().with_value(expected);
    }

    #[test]
    fn deserialize_block_list() {
        r##"
        <?xml version="1.0" encoding="utf-8"?>  
        <BlockList>  
          <Uncommitted>MA==</Uncommitted>  
          <Committed>MQ==</Committed>  
          <Latest>Mg==</Latest>  
        </BlockList>  "##
            .parse::<BlockList>()
            .should()
            .be_ok()
            .which_value()
            .blocks
            .should()
            .equal_iterator(vec![
                BlockId::Uncommitted("0".to_owned()),
                BlockId::Committed("1".to_owned()),
                BlockId::Latest("2".to_owned()),
            ]);
    }

    #[test]
    fn serialize_block_list() {
        let expected = BlockList {
            blocks: vec![
                BlockId::Uncommitted("0".to_owned()),
                BlockId::Committed("1".to_owned()),
                BlockId::Latest("2".to_owned()),
            ],
        };

        expected.to_string().parse::<BlockList>().should().be_ok().with_value(expected);
    }

    #[test]
    fn to_stream_info_converts_blob_to_stream_info() {
        let size: u64 = 100500;
        let creation_time = Utc::today().and_hms(1, 1, 1);
        let modified_time = Utc::today().and_hms(2, 2, 2);
        let uri = "https://someaccount.blob.core.windows.net/somecontainer";

        let properties = hashmap! {
            "Content-Length".to_string() => size.to_string(),
            "Creation-Time".to_string() => creation_time.to_rfc2822(),
            "Last-Modified".to_string() => modified_time.to_rfc2822()
        };
        let blob = Blob {
            name: "test_blob.bin".to_string(),
            properties,
            metadata: hashmap! {},
        };
        let request_builder = RequestBuilder::new(uri, Arc::new(())).expect("creating RequestBuilder should succeed");

        let stream_info = blob.to_stream_info(&request_builder, SyncRecord::empty());

        stream_info.resource_id().should().be(format!("{}/{}", uri, blob.name).as_str());
        stream_info
            .session_properties()
            .size()
            .should()
            .be_some()
            .which_value()
            .should()
            .be(&size);
        stream_info
            .session_properties()
            .created_time()
            .should()
            .be_some()
            .which_value()
            .should()
            .be(&creation_time);
        stream_info
            .session_properties()
            .modified_time()
            .should()
            .be_some()
            .which_value()
            .should()
            .be(&modified_time);
    }

    #[test]
    fn is_hdi_folder_returns_true_for_blob_marked_as_hdi_folder() {
        let blob = Blob {
            name: "test.csv".to_owned(),
            properties: hashmap! {},
            metadata: hashmap! {
                HDI_ISFOLDER_METADATA_NAME.to_owned() => "true".to_owned()
            },
        };

        blob.is_hdi_folder().should().be_true();
    }

    #[test]
    fn is_hdi_folder_returns_false_for_blob_not_marked_as_hdi_folder() {
        let blob = Blob {
            name: "test.csv".to_owned(),
            properties: hashmap! {},
            metadata: hashmap! {
                HDI_ISFOLDER_METADATA_NAME.to_owned() => "false".to_owned()
            },
        };
        blob.is_hdi_folder().should().be_false();

        let blob = Blob {
            name: "test.csv".to_owned(),
            properties: hashmap! {},
            metadata: hashmap! {
                HDI_ISFOLDER_METADATA_NAME.to_owned() => "random_metadata_value".to_owned()
            },
        };
        blob.is_hdi_folder().should().be_false();

        let blob = Blob {
            name: "test.csv".to_owned(),
            properties: hashmap! {},
            metadata: hashmap! {},
        };
        blob.is_hdi_folder().should().be_false();
    }
}
