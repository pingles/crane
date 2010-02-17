(ns crane.utils
 )
 
;; got xml renderer from http://www.erik-rasmussen.com/blog/2009/09/08/xml-renderer-in-clojure/
;; took out html stuff
(defn render-xml-attributes [attributes]
 (when attributes
   (apply str
     (for [[key value] attributes]
       (str \space (name key) "=\"" value \")))))
 
(defn render-xml [node]
   (if (string? node)
     (.trim node)
     (let [tag (:tag node)
           children (:content node)
           has-children? (not-empty children)
           open-tag (str \< (name tag)
                       (render-xml-attributes (:attrs node))
                       (if has-children? \> "/>"))
           close-tag (when has-children? (str "</" (name tag) \>))]
       (str
         open-tag
         (apply str (when has-children?
                      (for [child children]
                        (render-xml child))))
         close-tag))))