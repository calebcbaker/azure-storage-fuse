use minidom::Element;
use rslex_core::{
    file_io::{StreamError, StreamResult},
    MapErrToUnknown,
};
use std::{collections::HashMap, str::FromStr};

#[derive(Debug, Clone, PartialEq)]
/// File metadata as retrieved from the file service
pub(super) struct File {
    pub name: String,

    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct Directory {
    pub name: String,
}

/// result returned by "List Directories and Files"
/// https://docs.microsoft.com/en-us/rest/api/storageservices/list-directories-and-files
#[derive(Debug, PartialEq)]
pub(super) struct DirectoriesAndFiles {
    pub directory_path: String,

    pub files: Vec<File>,

    pub directories: Vec<Directory>,

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

impl FromStr for DirectoriesAndFiles {
    type Err = StreamError;

    fn from_str(s: &str) -> StreamResult<Self> {
        let root: Element = s.parse::<Element>().map_err_to_unknown()?;
        let mut files = vec![];
        let mut directories = vec![];

        for b in root
            .get_child_without_ns("Entries")
            .ok_or(StreamError::Unknown(
                "Azure File List DirectoriesAndFiles XML missing <Entries> node".to_owned(),
                None,
            ))?
            .children()
        {
            if b.name() == "File" {
                files.push(File {
                    name: b
                        .get_child_without_ns("Name")
                        .ok_or(StreamError::Unknown("AzureFile XML <File> missing <Name> node".to_owned(), None))?
                        .text(),
                    properties: match b.get_child_without_ns("Properties") {
                        Some(e) => e.children().map(|c| (c.name().to_owned(), c.text())).collect(),
                        None => HashMap::default(),
                    },
                });
            } else if b.name() == "Directory" {
                directories.push(Directory {
                    name: b
                        .get_child_without_ns("Name")
                        .ok_or(StreamError::Unknown(
                            "BlobList XML <Directory> missing <Name> node".to_owned(),
                            None,
                        ))?
                        .text(),
                });
            }
        }

        let directory_path = root.attr("DirectoryPath").unwrap_or("").to_owned();

        let next_marker = match root.get_child_without_ns("NextMarker") {
            Some(e) if e.text() == "" => None,
            Some(e) => Some(e.text()),
            None => None,
        };

        tracing::debug!(
            "[DirectoriesAndFiles::from_str()] deserialized, with {} files and {} directores",
            files.len(),
            directories.len()
        );

        Ok(DirectoriesAndFiles {
            directory_path,
            files,
            directories,
            next_marker,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluent_assertions::*;
    use maplit::hashmap;

    impl ToString for DirectoriesAndFiles {
        fn to_string(&self) -> String {
            let root = Element::builder("EnumerationResults")
                .attr("DirectoryPath", self.directory_path.as_str())
                .append(
                    Element::builder("Entries")
                        .append_all(self.files.iter().map(|file| {
                            Element::builder("File")
                                .append(Element::builder("Name").append(file.name.as_str()))
                                .append(
                                    Element::builder("Properties")
                                        .append_all(file.properties.iter().map(|kv| Element::builder(kv.0).append(kv.1.as_str()))),
                                )
                        }))
                        .append_all(
                            self.directories
                                .iter()
                                .map(|p| Element::builder("Directory").append(Element::builder("Name").append(p.name.as_str()))),
                        ),
                )
                .append(Element::builder("NextMarker").append(self.next_marker.as_ref().map(|s| s.as_str()).unwrap_or("")))
                .build();

            let mut output = vec![];
            root.write_to(&mut output).unwrap();
            String::from_utf8(output).unwrap()
        }
    }

    #[test]
    fn deserialize_directories_and_files_without_next_marker() {
        let expected = DirectoriesAndFiles {
            directory_path: "folder".to_owned(),
            files: vec![File {
                name: "PP0716.MUIDFLT33.txt".to_string(),
                properties: hashmap! { "Content-Length".to_owned() => "6282131".to_owned() },
            }],
            directories: vec![
                Directory { name: "input".to_string() },
                Directory {
                    name: "output".to_string(),
                },
            ],
            next_marker: None,
        };

        r##"
        <?xml version="1.0" encoding="utf-8"?>
        <EnumerationResults ServiceEndpoint="https://dataprepe2etest.file.core.windows.net/" ShareName="test" DirectoryPath="folder">
            <Entries>
                <Directory>
                    <Name>input</Name>
                    <Properties />
                </Directory>
                <Directory>
                    <Name>output</Name>
                    <Properties />
                </Directory>
                <File>
                    <Name>PP0716.MUIDFLT33.txt</Name>
                    <Properties>
                        <Content-Length>6282131</Content-Length>
                    </Properties>
                </File>
            </Entries>
            <NextMarker />
        </EnumerationResults>
        "##
        .parse::<DirectoriesAndFiles>()
        .should()
        .be_ok()
        .with_value(expected);
    }

    #[test]
    fn deserialize_directories_and_files_with_next_markers() {
        r##"
        <?xml version="1.0" encoding="utf-8"?>
        <EnumerationResults ServiceEndpoint="https://dataprepe2etest.file.core.windows.net/" ShareName="test" DirectoryPath="folder">
            <Entries>
                <Directory>
                    <Name>input</Name>
                    <Properties />
                </Directory>
                <Directory>
                    <Name>output</Name>
                    <Properties />
                </Directory>
                <File>
                    <Name>PP0716.MUIDFLT33.txt</Name>
                    <Properties>
                        <Content-Length>6282131</Content-Length>
                    </Properties>
                </File>
            </Entries>
            <NextMarker>2!76!MDAwMDEyIWhvdXNpbmcueGxzeCEwMDAwMjghOTk5OS0xMi0zMVQyMzo1OTo1OS45OTk5OTk5WiE-</NextMarker>
        </EnumerationResults>
        "##
        .parse::<DirectoriesAndFiles>()
        .should()
        .be_ok()
        .which_value()
        .next_marker
        .should()
        .be_some()
        .which_value()
        .should()
        .equal_string("2!76!MDAwMDEyIWhvdXNpbmcueGxzeCEwMDAwMjghOTk5OS0xMi0zMVQyMzo1OTo1OS45OTk5OTk5WiE-");
    }

    #[test]
    fn deserialize_empty_from_xml() {
        let expected = DirectoriesAndFiles {
            directory_path: "".to_owned(),
            files: vec![],
            directories: vec![],
            next_marker: None,
        };

        r##"
            <?xml version="1.0" encoding="utf-8"?>
            <EnumerationResults ServiceEndpoint="https://dataprepe2etest.blob.core.windows.net/" ContainerName="test">
                <Entries>
                </Entries>
            </EnumerationResults>
            "##
        .parse::<DirectoriesAndFiles>()
        .should()
        .be_ok()
        .with_value(expected);
    }

    #[test]
    fn roundtrip_directories_and_files_to_xml() {
        let expected = DirectoriesAndFiles {
            directory_path: "folder".to_owned(),
            files: vec![File {
                name: "PP0716.MUIDFLT33.txt".to_string(),
                properties: hashmap! { "Content-Length".to_owned() => "6282131".to_owned() },
            }],
            directories: vec![
                Directory { name: "input".to_string() },
                Directory {
                    name: "output".to_string(),
                },
            ],
            next_marker: Some("abcdefg".to_owned()),
        };

        expected
            .to_string()
            .parse::<DirectoriesAndFiles>()
            .should()
            .be_ok()
            .with_value(expected);
    }
}
