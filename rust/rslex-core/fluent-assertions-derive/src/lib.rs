use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{parse2, *};

#[proc_macro_derive(EnumAssertions)]
pub fn enum_assertions(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input: TokenStream = input.into();

    impl_enum_assertions(input).into()
}

fn pascal_case_to_snake_case(name: impl ToString) -> String {
    let mut result = String::default();
    let mut first = true;

    for char in name.to_string().chars() {
        if char.is_uppercase() {
            if !first {
                result.push('_');
            }
            result.extend(char.to_lowercase());
        } else {
            result.push(char);
        }
        first = false;
    }

    result
}

fn impl_enum_assertions(input: TokenStream) -> TokenStream {
    let item: ItemEnum = match parse2(input) {
        Ok(item) => item,
        Err(err) => {
            return err.to_compile_error();
        },
    };

    let vis = &item.vis;
    let class_name = &item.ident;
    let assertions_name = format_ident!("{}Assertions", item.ident);
    let constraint_name = format_ident!("{}Constraint", item.ident);

    let constraints = item.variants.iter().map(|v| {
        let variant_phantom_name = format_ident!("Is{}{}", item.ident, v.ident);
        let variant_name = &v.ident;
        match &v.fields {
            Fields::Unit => {
                quote! {
                    #vis struct #variant_phantom_name;
                }
            },
            Fields::Unnamed(uf) if uf.unnamed.len() == 1 => {
                let field = &uf.unnamed[0];
                let field_type = &field.ty;
                quote! {
                    #vis struct #variant_phantom_name;

                    impl #constraint_name<#class_name, #variant_phantom_name> {
                        pub fn which_value(self) -> #field_type {
                            if let #class_name::#variant_name(f) = self.subject {
                                f
                            } else {
                                panic!()
                            }
                        }
                    }

                    impl<'t> #constraint_name<&'t #class_name, #variant_phantom_name> {
                        pub fn which_value(self) -> &'t #field_type {
                            if let #class_name::#variant_name(f) = self.subject {
                                f
                            } else {
                                panic!()
                            }
                        }
                    }
                }
            },
            Fields::Unnamed(uf) => {
                let return_type = uf.unnamed.iter().map(|f| &f.ty).collect::<Vec<_>>();
                let inner_fields = (0..uf.unnamed.len()).map(|i| format_ident!("f{}", i)).collect::<Vec<_>>();
                quote! {
                    #vis struct #variant_phantom_name;

                    impl #constraint_name<#class_name, #variant_phantom_name> {
                        pub fn which_value(self) -> (#(#return_type),*) {
                            if let #class_name::#variant_name(#(#inner_fields),*) = self.subject {
                                (#(#inner_fields),*)
                            } else {
                                panic!()
                            }
                        }
                    }

                    impl<'t> #constraint_name<&'t #class_name, #variant_phantom_name> {
                        pub fn which_value(self) -> (#(&'t #return_type),*) {
                            if let #class_name::#variant_name(#(#inner_fields),*) = self.subject {
                                (#(#inner_fields),*)
                            } else {
                                panic!()
                            }
                        }
                    }
                }
            },
            Fields::Named(nf) => {
                let field_names = nf.named.iter().map(|f| &f.ident).collect::<Vec<_>>();
                let field_types = nf.named.iter().map(|f| &f.ty).collect::<Vec<_>>();
                let return_type = format_ident!("{}{}Struct", &item.ident, &v.ident);
                let return_type_ref = format_ident!("{}{}StructRef", &item.ident, &v.ident);
                quote! {
                    #vis struct #variant_phantom_name;

                    #[allow(unused_variables, dead_code)]
                    #vis struct #return_type {
                        #(pub #field_names: #field_types),*
                    }

                    #[allow(unused_variables, dead_code)]
                    #vis struct #return_type_ref<'t> {
                        #(pub #field_names: &'t #field_types),*
                    }

                    impl #constraint_name<#class_name, #variant_phantom_name> {
                        pub fn which_value(self) -> #return_type {
                            if let #class_name::#variant_name{#(#field_names),*} = self.subject {
                                #return_type { #(#field_names),* }
                            } else {
                                panic!()
                            }
                        }
                    }

                    impl<'t> #constraint_name<&'t #class_name, #variant_phantom_name> {
                        pub fn which_value(self) -> #return_type_ref<'t> {
                            if let #class_name::#variant_name{#(#field_names),*} = self.subject {
                                #return_type_ref { #(#field_names),* }
                            } else {
                                panic!()
                            }
                        }
                    }
                }
            },
        }
    });

    let be_funs = item.variants.iter().map(|v| {
        let variant_phantom_name = format_ident!("Is{}{}", item.ident, v.ident);
        let fun_name = format_ident!("be_{}", pascal_case_to_snake_case(&v.ident));
        quote! {
            fn #fun_name(self) -> #constraint_name<T, #variant_phantom_name>;
        }
    });

    let impl_funs = item.variants.iter().map(|v| {
        let variant_phantom_name = format_ident!("Is{}{}", item.ident, v.ident);
        let fun_name = format_ident!("be_{}", pascal_case_to_snake_case(&v.ident));
        let variant_name = &v.ident;

        let match_pattern = match &v.fields {
            Fields::Unit => quote! { #class_name::#variant_name },
            Fields::Unnamed(uf) => {
                let len = uf.unnamed.len();
                let filler = vec![quote! {_}; len];
                quote! { #class_name::#variant_name(#(#filler),*) }
            },
            Fields::Named(nf) => {
                let inner_fields = nf.named.iter().map(|f| &f.ident);
                quote! { #class_name::#variant_name{#(#inner_fields:_),*} }
            },
        };

        quote! {
            fn #fun_name(self) -> #constraint_name<T, #variant_phantom_name> {
                if let #match_pattern = self.subject().borrow() {
                } else {
                    fluent_assertions::utils::AssertionFailure::new(format!("should().{} expecting subject to be {}::{} but failed",
                        stringify!(#fun_name), stringify!(#class_name), stringify!(#variant_name)))
                        .subject(self.subject())
                        .fail();
                }

                #constraint_name::new(self.into_inner(), #variant_phantom_name)
            }
        }
    });

    let gen = quote! {
        #vis struct #constraint_name<T, V> {
            subject: T,
            variant: V,
        }

        impl<T: std::borrow::Borrow<#class_name>, V> #constraint_name<T, V> {
            pub fn new(subject: T, variant: V) -> Self {
                #constraint_name { subject, variant }
            }

            pub fn and(self) -> T {
                self.subject
            }
        }

        #(#constraints)*

        #vis trait #assertions_name<T> {
            #(#be_funs)*
        }

        impl<T:std::borrow::Borrow<#class_name>> #assertions_name<T> for fluent_assertions::Assertions<T> {
            #(#impl_funs)*
        }
    };

    // println!("{}", &gen);

    gen
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_word_to_snake() {
        assert_eq!(pascal_case_to_snake_case("Some"), "some");
    }

    #[test]
    fn double_words_to_snake() {
        assert_eq!(pascal_case_to_snake_case("SomeWord"), "some_word");
    }
}
